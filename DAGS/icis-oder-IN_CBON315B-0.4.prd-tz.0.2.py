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
                , WORKFLOW_NAME='IN_CBON315B',WORKFLOW_ID='d34e26c7b79f41fca59e472c2254511b', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBON315B-0.4.prd-tz.0.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 10, 10, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('d34e26c7b79f41fca59e472c2254511b')

    cbon315bJob_vol = []
    cbon315bJob_volMnt = []
    cbon315bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon315bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon315bJob_env = [getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISConfigMap('icis-oder-intelnet-batch-configmap2'), getICISSecret('icis-oder-intelnet-batch-secret')]
    cbon315bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon315bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon315bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5d6ebec34b1d4a87aa2555dc75695d20',
        'volumes': cbon315bJob_vol,
        'volume_mounts': cbon315bJob_volMnt,
        'env_from':cbon315bJob_env,
        'task_id':'cbon315bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.1',
        'arguments':["--job.name=cbon315bJob", "endTranDate=20241104", "ofcCd=710571", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('d34e26c7b79f41fca59e472c2254511b')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon315bJob,
        Complete
    ]) 

    # authCheck >> cbon315bJob >> Complete
    workflow








