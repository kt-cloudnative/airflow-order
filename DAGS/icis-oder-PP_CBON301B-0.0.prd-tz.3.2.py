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
                , WORKFLOW_NAME='PP_CBON301B',WORKFLOW_ID='f188ee13717d4c69b763328fa72b2da5', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON301B-0.0.prd-tz.3.2'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 19, 20, 51, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f188ee13717d4c69b763328fa72b2da5')

    cbon301bJob_vol = []
    cbon301bJob_volMnt = []
    cbon301bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon301bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon301bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbon301bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon301bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon301bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e7987033d5504342ad65065facc4cb5d',
        'volumes': cbon301bJob_vol,
        'volume_mounts': cbon301bJob_volMnt,
        'env_from':cbon301bJob_env,
        'task_id':'cbon301bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=cbon301bJob" , "endTranDate=20240528","ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f188ee13717d4c69b763328fa72b2da5')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon301bJob,
        Complete
    ]) 

    # authCheck >> cbon301bJob >> Complete
    workflow








