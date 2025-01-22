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
                , WORKFLOW_NAME='PP_CBON305B',WORKFLOW_ID='8928160be9074cf3a70da389bb123034', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON305B-0.0.prd-tz.5.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 12, 9, 21, 5, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('8928160be9074cf3a70da389bb123034')

    cbon305bJob_vol = []
    cbon305bJob_volMnt = []
    cbon305bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon305bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon305bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbon305bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon305bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon305bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '3d03e0cad93c4eecb6799d3aa842df4e',
        'volumes': cbon305bJob_vol,
        'volume_mounts': cbon305bJob_volMnt,
        'env_from':cbon305bJob_env,
        'task_id':'cbon305bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.4',
        'arguments':["--job.name=cbon305bJob", "endTranDate=20241209", "ver="+str(datetime.now()), "requestDate=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('8928160be9074cf3a70da389bb123034')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon305bJob,
        Complete
    ]) 

    # authCheck >> cbon305bJob >> Complete
    workflow








