from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='ET_CSNB043B',WORKFLOW_ID='65bd085c34ed486a9a9682d5e516431c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CSNB043B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 3, 16, 45, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('65bd085c34ed486a9a9682d5e516431c')

    csnb043bJob_vol = []
    csnb043bJob_volMnt = []
    csnb043bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csnb043bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csnb043bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnb043bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    csnb043bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnb043bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9b6b46e1f8cc4d07991d772432af606c',
        'volumes': csnb043bJob_vol,
        'volume_mounts': csnb043bJob_volMnt,
        'env_from':csnb043bJob_env,
        'task_id':'csnb043bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.6',
        'arguments':["--job.name=csnb043bJob", "inputFile=ET01.KTKT.20241221", "inNpMob=1",  "inProcGb=A", "inAddFlg=N", "regEmpNo=91108904", "regOfcCd=711826", "regEmpName=csnb043b", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('65bd085c34ed486a9a9682d5e516431c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnb043bJob,
        Complete
    ]) 

    # authCheck >> csnb043bJob >> Complete
    workflow








