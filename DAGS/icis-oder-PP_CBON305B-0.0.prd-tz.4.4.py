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
                , WORKFLOW_NAME='PP_CBON305B',WORKFLOW_ID='580acd19a4e347e38458051321384019', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CBON305B-0.0.prd-tz.4.4'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 22, 16, 5, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('580acd19a4e347e38458051321384019')

    cbon305bJob_vol = []
    cbon305bJob_volMnt = []
    cbon305bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbon305bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbon305bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    cbon305bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbon305bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbon305bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'c08bdf50d9404849b78ed9565f2ccb46',
        'volumes': cbon305bJob_vol,
        'volume_mounts': cbon305bJob_volMnt,
        'env_from':cbon305bJob_env,
        'task_id':'cbon305bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=cbon305bJob", "endTranDate=20240530", "ver="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('580acd19a4e347e38458051321384019')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbon305bJob,
        Complete
    ]) 

    # authCheck >> cbon305bJob >> Complete
    workflow








